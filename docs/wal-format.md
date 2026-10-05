# WAL Format — v1

Status: draft. No backward-compatibility obligations before first release; any format change bumps the version.

## Conventions

- **Endianness:** big-endian for all multi-byte integers.
- **Checksum:** CRC32C (Castagnoli, reflected poly `0x82F63B78`), via Go `crc32.MakeTable(crc32.Castagnoli)`. Fixed for the life of a version.
- **Offsets** are from the start of the file.
- **Policy:** the decoder rejects anything it does not understand. A WAL is a durability structure; never guess.

## File layout

```
[ Header (16 B) ][ Record ][ Record ] ... 
```

First record starts at offset 16.

## Header (20 bytes)

| Offset | Size | Field      | Value / rule                                              |
| -----: | ---: | ---------- | --------------------------------------------------------- |
|      0 |    4 | Magic      | `52 4B 56 57` (`"RKVW"`)                                  |
|      4 |    2 | Version    | `0x0001`. Any other value → reject.                       |
|      6 |    2 | Reserved   | Must be all zero. Non-zero → reject.                      |
|      8 |    8 | BaseOffset | Logical offset of the first record in this file. New WAL = 0. |
|     16 |    4 | Header CRC | CRC32C over bytes 0–15                                    |

Why: magic rejects foreign files; version is the upgrade path; header CRC
separates "not our file" (bad magic) from "our file, damaged" (bad CRC).
BaseOffset lets WAL truncation rewrite the file without invalidating
logical offsets stored in snapshots.

## Record

| Field   |   Size | Notes                                             |
| ------- | -----: | ------------------------------------------------- |
| CRC     |      4 | CRC32C over **Length + Type + Payload**           |
| Length  |      4 | Payload byte count only (excludes Type and CRC)   |
| Type    |      1 | `1` = Put, `2` = Delete. Anything else → invalid. |
| Payload | Length | Shape depends on Type                             |

Total record size = `4 + 4 + 1 + Length`.

CRC covers Length so a corrupted length is detected before it misleads the reader.

### Payload shapes

| Type   | Payload                               | Length formula        |
| ------ | ------------------------------------- | --------------------- |
| Put    | `keyLen(u32) key valueLen(u32) value` | `8 + keyLen + valLen` |
| Delete | `keyLen(u32) key`                     | `4 + keyLen`          |

Strict: inner lengths must account for the payload **exactly**. Any mismatch or trailing byte → `ErrInvalid`, even if the CRC passes.

## Limits

| Constant       | Value                       | Derivation                                         |
| -------------- | --------------------------- | -------------------------------------------------- |
| `MaxPayload`   | 16 MiB (16,777,216)         | Package constant, not stored in the header         |
| `MaxKeySize`   | 1 KiB (1,024)               | Chosen                                             |
| `MaxValueSize` | 16 MiB − 4 KiB (16,773,120) | Keeps `8 + MaxKeySize + MaxValueSize ≤ MaxPayload` |

- Empty key is invalid. Empty value is legal for Put (distinct from Delete).
- Min valid Length: Delete = 5, Put = 9. `Length < 5` or `Length > MaxPayload` → `ErrInvalid`. This makes zeroed regions (`Length = 0`, `Type = 0`) invalid.
- Validate `Length` **before** allocating. Do size arithmetic in `uint64`/`int` after the cap check (no `uint32` overflow).
- Storage layer must enforce `MaxKeySize` / `MaxValueSize` before a write reaches the WAL.

## Decoder order (per record)

1. Read 8 bytes (CRC + Length) with `io.ReadFull`. 0 bytes → `io.EOF`; 1–7 → `ErrTruncated`.
2. Validate `Length` against limits.
3. Read `Type + Payload` (`1 + Length` bytes) with `io.ReadFull`. Short → `ErrTruncated`.
4. Verify CRC. Mismatch → `ErrChecksum`.
5. Validate Type, then parse payload strictly. Failure → `ErrInvalid`.

## Error taxonomy

The decoder **reports**; the recovery layer (Phase 1, task 5) **decides**.

| Error          | Meaning                           | Recovery action                                                                              |
| -------------- | --------------------------------- | -------------------------------------------------------------------------------------------- |
| `io.EOF`       | Clean end at a record boundary    | Done                                                                                         |
| `ErrTruncated` | Partial record at tail            | Discard, truncate file to last good offset                                                   |
| `ErrChecksum`  | Full record present, CRC mismatch | Last record → likely torn write, discard. Valid data after it → real corruption, fail loudly |
| `ErrInvalid`   | Bad type / length / payload shape | Same handling as `ErrChecksum`                                                               |

Header errors: `ErrBadMagic`, `ErrUnsupportedVersion`, `ErrHeaderChecksum`, `ErrTruncated`, `ErrInvalid` (non-zero reserved). All fatal except as covered below.

## File-level edge cases

| Situation                   | Handling                                                                 |
| --------------------------- | ------------------------------------------------------------------------ |
| 0-byte file                 | Fresh WAL: write header                                                  |
| 1–15 byte file              | Torn header (crash during creation); no records possible, rewrite header |
| Header only                 | Valid, zero records                                                      |
| Zeroed region after records | `ErrInvalid` (Length 0)                                                  |

## Write rules (enforced in task 3)

- **One record = one contiguous buffer = one `write()` call.** Build `Length | Type | Payload`, compute CRC, prepend. This shrinks the torn-write window; it does not eliminate it, which is why the CRC exists.
- **Header init:** on a 0-byte file, write header, `fsync` the file, then `fsync` the **parent directory** so the file's existence is durable. Only then accept records.

## Worked example (golden test vectors)

Header:

```
52 4B 56 57 | 00 01 | 00 00 | 00 00 00 00 00 00 00 00 | 08 81 32 B5
```

`Put("a", "b")` — payload `00000001 61 00000001 62` (10 bytes), Length = `0x0000000A`:

```
B7 62 2D 95 | 00 00 00 0A | 01 | 00 00 00 01 61 00 00 00 01 62
```

`Delete("a")` — payload `00000001 61` (5 bytes), Length = `0x00000005`:

```
75 BD 31 A7 | 00 00 00 05 | 02 | 00 00 00 01 61
```

Sanity check for CRC32C implementation: `CRC32C("123456789") = 0xE3069283`.