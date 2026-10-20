# WAL Format - v1

Status: draft. No backward-compatibility obligations before first release; any format change bumps the version.

## Conventions

- Big-endian for all multi-byte integers.
- Checksum: CRC32C (Castagnoli), via `crc32.MakeTable(crc32.Castagnoli)`.
- Offsets in this document are from the start of the file.
- The decoder rejects anything it does not understand. A WAL is a durability
  structure; never guess.

## File layout

```
[ Header (20 B) ][ Record ][ Record ] ...
```

The first record starts at offset 20.

## Header (20 bytes)

| Offset | Size | Field      | Rule                                                   |
| -----: | ---: | ---------- | ------------------------------------------------------ |
|      0 |    4 | Magic      | `52 4B 56 57` (`"RKVW"`)                               |
|      4 |    2 | Version    | `0x0001`; any other value is rejected                  |
|      6 |    2 | Reserved   | Must be zero                                           |
|      8 |    8 | BaseOffset | Logical offset of the first record in this file; new WAL = 0 |
|     16 |    4 | Header CRC | CRC32C over bytes 0-15                                 |

`BaseOffset` lets truncation rewrite the file without changing the logical
offsets stored in snapshots. A logical offset counts record bytes only
(headers excluded):

```
physical position = 20 + (logical offset - BaseOffset)
```

## Record

| Field   |   Size | Notes                                           |
| ------- | -----: | ----------------------------------------------- |
| CRC     |      4 | CRC32C over Length + Type + Payload             |
| Length  |      4 | Payload bytes only (excludes CRC and Type)      |
| Type    |      1 | `1` = Put, `2` = Delete; anything else is invalid |
| Payload | Length | Shape depends on Type                           |

Total record size = `9 + Length`. The CRC covers Length so a corrupted length
is caught before it misleads the reader.

| Type   | Payload                               | Length                |
| ------ | ------------------------------------- | --------------------- |
| Put    | `keyLen(u32) key valueLen(u32) value` | `8 + keyLen + valLen` |
| Delete | `keyLen(u32) key`                     | `4 + keyLen`          |

Inner lengths must account for the payload exactly. Any mismatch or trailing
byte is `ErrMalformed`, even if the CRC passes.

## Limits

| Constant       | Value                       |
| -------------- | --------------------------- |
| `MaxPayload`   | 16 MiB (package constant)   |
| `MaxKeySize`   | 1 KiB                       |
| `MaxValueSize` | 16 MiB - 4 KiB              |

- An empty key is rejected on write (`ErrEmptyKey`). An empty value is legal
  for Put and is not the same as Delete.
- Valid `Length` is 5 to `MaxPayload`. This makes a zeroed region (`Length = 0`)
  invalid.
- `Length` is validated before any allocation.
- Key and value limits are enforced before a record is encoded, so oversized
  writes never reach the file (`ErrTooLarge`).

## Decoding a record

1. Read the 9-byte prefix (CRC, Length, Type). Zero bytes gives `io.EOF`;
   1-8 bytes gives `io.ErrUnexpectedEOF`.
2. Validate `Length`. Failure is `ErrBadLength`.
3. Read `Length` payload bytes. A short read is `io.ErrUnexpectedEOF`.
4. Verify the CRC. Mismatch is `ErrChecksum`.
5. Validate Type (`ErrBadType`), then parse the payload (`ErrMalformed`).

## Errors and recovery

The decoder reports; `Recover` decides.

| Error                 | Meaning                              | Recovery action                                           |
| --------------------- | ------------------------------------ | --------------------------------------------------------- |
| `io.EOF`              | Clean end at a record boundary       | Done                                                      |
| `io.ErrUnexpectedEOF` | File ends mid-record                 | Torn tail: truncate to the last good offset               |
| `ErrChecksum`         | Full record present, CRC mismatch    | Torn tail only if it is the last record; otherwise fail   |
| `ErrBadLength`        | Length out of range                  | Torn tail only if the rest of the file is all zeros; otherwise fail |
| `ErrBadType`, `ErrMalformed` | Valid CRC, bad content        | Always fail (never a crash artifact)                      |

A torn tail is truncated and fsynced before any new append is accepted.
Corruption anywhere else fails startup.

Other errors: `ErrBadHeader` (bad magic, version, header CRC or short header),
`ErrBadOffset` (replay offset outside the WAL), `ErrBroken`, `ErrClosed`.

## File-level cases

| Situation                    | Handling                                                              |
| ---------------------------- | --------------------------------------------------------------------- |
| 0-byte file                  | New WAL: write header, fsync file, fsync parent directory             |
| 1-19 bytes                   | Must be a prefix of the expected header (crash during creation); rewrite the header. Otherwise `ErrBadHeader` |
| Header only                  | Valid, zero records                                                   |
| Zeroed region after records  | Torn tail (all zeros) and is truncated                                |

## Write rules

- One record = one contiguous buffer = one `write()`. This shrinks the torn-write
  window; the CRC covers what remains.
- `Append` returns only after the record is fsynced.
- After a failed write or fsync the WAL is **broken**: every later append
  returns `ErrBroken` until the process restarts and recovers.
- `WithGroupCommit()` lets concurrent appends share one fsync. Each append still
  returns only after its own record is durable.
- `Recover(from)` replays records at logical offset `>= from`. `from` must be
  within `[BaseOffset, end of file]`, otherwise `ErrBadOffset`.

## Truncation

After a snapshot with `WALOffset = S` is fully durable, records before S are
redundant. `TruncateBefore(S)`:

1. Create `<wal>.tmp` with a header whose `BaseOffset = S`.
2. Copy the record bytes from logical offset S to the end.
3. fsync the tmp file.
4. Rename it over the WAL.
5. fsync the parent directory.

- Truncation starts only after the snapshot (file, rename, directory fsync) is
  durable. Any snapshot error means no truncation.
- Logical offsets never change, so a snapshot stays valid before, during and
  after truncation. A crash at any step leaves the old or the new WAL, and
  recovery is correct for both.
- If the directory fsync fails after the rename, the WAL is marked broken.
- `<wal>.tmp` is deleted on open.

## Golden test vectors

Header (`BaseOffset = 0`):

```
52 4B 56 57 | 00 01 | 00 00 | 00 00 00 00 00 00 00 00 | 08 81 32 B5
```

`Put("a", "b")`, Length = `0x0A`:

```
B7 62 2D 95 | 00 00 00 0A | 01 | 00 00 00 01 61 00 00 00 01 62
```

`Delete("a")`, Length = `0x05`:

```
75 BD 31 A7 | 00 00 00 05 | 02 | 00 00 00 01 61
```

CRC32C check value: `CRC32C("123456789") = 0xE3069283`.

## Benchmark

AMD Ryzen 7 6800HS, Linux, amd64:

| Mode  | Goroutines | ns/op     | Records per fsync |
| ----- | ---------: | --------: | ----------------: |
| sync  |          1 | 2,151,056 | 1.0               |
| sync  |         64 | 2,139,492 | 1.0               |
| group |          1 | 2,215,558 | 1.0               |
| group |          8 |   537,076 | 4.0               |
| group |         64 |    77,397 | 32.7              |

A synchronous append costs about one fsync (~2.1 ms). Group commit amortizes
it across concurrent writers. The engine currently uses the synchronous path.