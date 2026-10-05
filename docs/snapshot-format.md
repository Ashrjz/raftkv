# Snapshot Format (v1)

A snapshot is a point-in-time copy of the full key-value map, plus the WAL
position it covers. Recovery loads the snapshot, then replays only the WAL
records after that position.

## Conventions

- Big-endian, fixed-width integers (same as the WAL).
- Checksum: CRC32C (Castagnoli), same as the WAL.
- Key/value size limits are the WAL's constants; any pair valid in the WAL
  fits in a snapshot.

## Files

| Name           | Purpose                                   |
|----------------|-------------------------------------------|
| `snapshot`     | Current snapshot (at most one).           |
| `snapshot.tmp` | In-progress write; never read, deleted on startup. |

## Layout

```
+---------------------------+
| Header           (12 B)   |
| Metadata         (16 B)   |
| Entry 0 .. Entry N-1      |
| Footer            (4 B)   |
+---------------------------+
```

### Header (12 bytes)

| Field    | Size | Notes                                   |
|----------|------|-----------------------------------------|
| Magic    | 8    | `RKVSNAP\0`                             |
| Version  | 2    | `1`; unknown version is rejected        |
| Reserved | 2    | Must be 0; non-zero is rejected         |

### Metadata (16 bytes)

| Field      | Size | Notes                                                        |
|------------|------|--------------------------------------------------------------|
| WALOffset  | 8    | Logical WAL offset covered by this snapshot (see below)      |
| EntryCount | 8    | Number of entries that follow                                |

### Entry

| Field    | Size     | Notes                                |
|----------|----------|--------------------------------------|
| KeyLen   | 4        | Bounded by the WAL's key limit       |
| ValueLen | 4        | Bounded by the WAL's value limit     |
| Key      | KeyLen   |                                      |
| Value    | ValueLen |                                      |

Entries are in no defined order. Readers must not depend on order.
Duplicate keys are corruption.

### Footer (4 bytes)

CRC32C over every preceding byte of the file (header, metadata, all entries).

## WAL position

`WALOffset` is a **logical** offset: the total number of record bytes ever
appended to the WAL, excluding headers. It is unaffected by WAL truncation.

The WAL header carries `BaseOffset`, the logical offset of its first record.
Physical replay start:

```
headerSize + (WALOffset - BaseOffset)
```

- `WALOffset < BaseOffset`: WAL was truncated past the snapshot. Corruption;
  fail startup.
- `WALOffset` beyond the end of the WAL: corruption; fail startup.

## Writing (atomic)

1. Under the engine lock, capture the map and the current logical WAL offset
   together (log-before-apply under one lock keeps them consistent).
2. Write the full file to `snapshot.tmp`.
3. `fsync` `snapshot.tmp`.
4. `rename` it over `snapshot`.
5. `fsync` the parent directory.

A crash before step 4 leaves the previous snapshot (or none) and the full WAL.
A crash after step 4 leaves the new snapshot and an untruncated WAL. Both
states recover correctly.

## Loading and validation

Loader rejects (startup fails, no fallback to full WAL replay) if:

- magic, version, or reserved field is invalid
- any `KeyLen` / `ValueLen` exceeds its limit (checked **before** allocating)
- the file ends early, or has bytes between the last entry and the footer
- entries read != `EntryCount`
- a duplicate key is found
- the footer CRC does not match

Entries are decoded into a temporary map; the engine's map is replaced only
after the CRC verifies.

## Recovery order

1. Delete `snapshot.tmp` if present.
2. If `snapshot` exists: load and validate it, then set the map.
3. Replay the WAL from the physical position derived above.
4. Torn-tail handling is unchanged from the WAL spec.

## Non-goals (v1)

- Incremental or compressed snapshots.
- Multiple retained snapshots.
- Automatic snapshot triggers (explicit `Snapshot()` call only).
- Backward compatibility.