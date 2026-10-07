package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// ---------- helpers ----------

func snapHex(t *testing.T, parts ...string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.Join(parts, ""))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// snapReseal recomputes the footer CRC over everything before the last 4 bytes,
// so a test can corrupt one field and still reach the validation it targets.
func snapReseal(b []byte) []byte {
	n := len(b) - snapFooterSize
	binary.BigEndian.PutUint32(b[n:], crc32.Checksum(b[:n], castagnoli))
	return b
}

func snapEntry(k, v []byte) []byte {
	b := make([]byte, 8, 8+len(k)+len(v))
	binary.BigEndian.PutUint32(b[0:4], uint32(len(k)))
	binary.BigEndian.PutUint32(b[4:8], uint32(len(v)))
	return append(append(b, k...), v...)
}

// snapBuild builds a structurally controllable snapshot with a valid CRC.
func snapBuild(walOffset, count uint64, entries ...[]byte) []byte {
	b := make([]byte, snapHeaderSize+snapMetaSize)
	copy(b[0:8], snapMagic[:])
	binary.BigEndian.PutUint16(b[8:10], snapVersion)
	binary.BigEndian.PutUint64(b[12:20], walOffset)
	binary.BigEndian.PutUint64(b[20:28], count)
	for _, e := range entries {
		b = append(b, e...)
	}
	return snapReseal(append(b, 0, 0, 0, 0))
}

func snapPut(t *testing.T, dir string, b []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, SnapshotFile), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func snapMapsEqual(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok || !bytes.Equal(av, bv) { // nil and empty are equal here
			return false
		}
	}
	return true
}

func snapExists(dir, name string) bool {
	_, err := os.Stat(filepath.Join(dir, name))
	return err == nil
}

// ---------- golden vectors ----------

func TestSnapshotGolden(t *testing.T) {
	tests := []struct {
		name string
		data map[string][]byte
		off  uint64
		want []byte
	}{
		{
			name: "empty",
			data: map[string][]byte{},
			off:  0,
			want: snapHex(t, "524B56534E415000", "0001", "0000",
				"0000000000000000", "0000000000000000", "3072A92E"),
		},
		{
			name: "put_a_b_offset10",
			data: map[string][]byte{"a": []byte("b")},
			off:  10,
			want: snapHex(t, "524B56534E415000", "0001", "0000",
				"000000000000000A", "0000000000000001",
				"00000001", "00000001", "6162", "4E7C25F1"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := WriteSnapshot(dir, tt.data, tt.off); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(filepath.Join(dir, SnapshotFile))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("encode mismatch:\n got  %X\n want %X", got, tt.want)
			}

			// And the golden bytes decode back to the same content.
			dir2 := t.TempDir()
			snapPut(t, dir2, tt.want)
			m, off, found, err := LoadSnapshot(dir2)
			if err != nil || !found || off != tt.off || !snapMapsEqual(m, tt.data) {
				t.Fatalf("decode: m=%v off=%d found=%v err=%v", m, off, found, err)
			}
		})
	}
}

// ---------- round trips ----------

func TestSnapshotRoundTrip(t *testing.T) {
	many := make(map[string][]byte)
	for i := 0; i < 1000; i++ {
		many["key-"+strconv.Itoa(i)] = []byte("val-" + strconv.Itoa(i))
	}

	tests := []struct {
		name string
		data map[string][]byte
		off  uint64
	}{
		{"empty_map", map[string][]byte{}, 0},
		{"single", map[string][]byte{"a": []byte("b")}, 42},
		{"nil_and_empty_values", map[string][]byte{"n": nil, "e": {}}, 7},
		{"binary_key_and_value", map[string][]byte{"\x00\xff\x00": {0, 1, 2, 0xff}}, 1},
		{"max_key", map[string][]byte{strings.Repeat("k", MaxKeySize): []byte("v")}, 1},
		{"large_value_1MiB", map[string][]byte{"big": bytes.Repeat([]byte{0xAB}, 1<<20)}, 1 << 33},
		{"many_keys", many, 123456},
		{"max_uint64_offset", map[string][]byte{"a": []byte("b")}, ^uint64(0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := WriteSnapshot(dir, tt.data, tt.off); err != nil {
				t.Fatal(err)
			}
			m, off, found, err := LoadSnapshot(dir)
			if err != nil || !found {
				t.Fatalf("load: found=%v err=%v", found, err)
			}
			if off != tt.off {
				t.Fatalf("walOffset = %d, want %d", off, tt.off)
			}
			if !snapMapsEqual(m, tt.data) {
				t.Fatalf("data mismatch: got %d keys, want %d", len(m), len(tt.data))
			}
		})
	}
}

func TestSnapshotEmptyValueLoadsAsNil(t *testing.T) {
	dir := t.TempDir()
	if err := WriteSnapshot(dir, map[string][]byte{"k": {}}, 0); err != nil {
		t.Fatal(err)
	}
	m, _, _, err := LoadSnapshot(dir)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := m["k"]; !ok || v != nil {
		t.Fatalf("got %#v, ok=%v; want nil value present (matches WAL replay)", v, ok)
	}
}

// ---------- missing / tmp handling ----------

func TestLoadSnapshotMissing(t *testing.T) {
	m, off, found, err := LoadSnapshot(t.TempDir())
	if err != nil || found || m != nil || off != 0 {
		t.Fatalf("got m=%v off=%d found=%v err=%v; want nothing found, no error", m, off, found, err)
	}
}

func TestLoadSnapshotIgnoresTmp(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, SnapshotTmpFile), snapBuild(0, 0), 0o644); err != nil {
		t.Fatal(err)
	}
	_, _, found, err := LoadSnapshot(dir)
	if err != nil || found {
		t.Fatalf("found=%v err=%v; snapshot.tmp must never be read", found, err)
	}
}

func TestRemoveStaleSnapshotTmp(t *testing.T) {
	dir := t.TempDir()

	// No-op when absent.
	if err := RemoveStaleSnapshotTmp(dir); err != nil {
		t.Fatalf("absent tmp: %v", err)
	}

	// Removes tmp, leaves the real snapshot alone.
	if err := WriteSnapshot(dir, map[string][]byte{"a": []byte("b")}, 5); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, SnapshotTmpFile), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := RemoveStaleSnapshotTmp(dir); err != nil {
		t.Fatal(err)
	}
	if snapExists(dir, SnapshotTmpFile) {
		t.Fatal("snapshot.tmp still present")
	}
	if _, _, found, err := LoadSnapshot(dir); err != nil || !found {
		t.Fatalf("real snapshot damaged: found=%v err=%v", found, err)
	}
}

// ---------- write behavior ----------

func TestWriteSnapshotReplacesPrevious(t *testing.T) {
	dir := t.TempDir()
	a := map[string][]byte{"a": []byte("1")}
	b := map[string][]byte{"b": []byte("2"), "c": []byte("3")}

	if err := WriteSnapshot(dir, a, 1); err != nil {
		t.Fatal(err)
	}
	if err := WriteSnapshot(dir, b, 2); err != nil {
		t.Fatal(err)
	}
	m, off, found, err := LoadSnapshot(dir)
	if err != nil || !found || off != 2 || !snapMapsEqual(m, b) {
		t.Fatalf("m=%v off=%d found=%v err=%v; want second snapshot", m, off, found, err)
	}
	if snapExists(dir, SnapshotTmpFile) {
		t.Fatal("snapshot.tmp left behind after successful write")
	}
}

func TestWriteSnapshotOverwritesStaleTmp(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, SnapshotTmpFile), bytes.Repeat([]byte("x"), 10000), 0o644); err != nil {
		t.Fatal(err)
	}
	data := map[string][]byte{"a": []byte("b")}
	if err := WriteSnapshot(dir, data, 3); err != nil {
		t.Fatal(err)
	}
	m, off, found, err := LoadSnapshot(dir)
	if err != nil || !found || off != 3 || !snapMapsEqual(m, data) {
		t.Fatalf("m=%v off=%d found=%v err=%v", m, off, found, err)
	}
}

func TestWriteSnapshotRejectsInvalidData(t *testing.T) {
	tests := []struct {
		name string
		data map[string][]byte
		want error
	}{
		{"empty_key", map[string][]byte{"ok": []byte("1"), "": []byte("x")}, ErrEmptyKey},
		{"key_too_large", map[string][]byte{strings.Repeat("k", MaxKeySize+1): nil}, ErrTooLarge},
		{"value_too_large", map[string][]byte{"k": make([]byte, MaxValueSize+1)}, ErrTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			prev := map[string][]byte{"keep": []byte("me")}
			if err := WriteSnapshot(dir, prev, 9); err != nil {
				t.Fatal(err)
			}

			err := WriteSnapshot(dir, tt.data, 99)
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			if snapExists(dir, SnapshotTmpFile) {
				t.Fatal("snapshot.tmp left behind after failed write")
			}
			// The previous snapshot must be untouched.
			m, off, found, lerr := LoadSnapshot(dir)
			if lerr != nil || !found || off != 9 || !snapMapsEqual(m, prev) {
				t.Fatalf("previous snapshot damaged: m=%v off=%d found=%v err=%v", m, off, found, lerr)
			}
		})
	}
}

// ---------- corruption: targeted structural cases (CRC kept valid) ----------

func TestLoadSnapshotRejectsStructuralCorruption(t *testing.T) {
	good := func() []byte { return snapBuild(5, 1, snapEntry([]byte("a"), []byte("b"))) }

	tests := []struct {
		name  string
		build func() []byte
	}{
		{"bad_magic", func() []byte { b := good(); b[0] ^= 0xFF; return snapReseal(b) }},
		{"bad_version", func() []byte { b := good(); b[9] = 2; return snapReseal(b) }},
		{"reserved_nonzero_first", func() []byte { b := good(); b[10] = 1; return snapReseal(b) }},
		{"reserved_nonzero_second", func() []byte { b := good(); b[11] = 1; return snapReseal(b) }},

		{"count_impossible_for_file_size", func() []byte { return snapBuild(0, 1<<40) }},
		{"count_exceeds_entries_present", func() []byte {
			// One 100-byte entry, count says 2: passes the size sanity check,
			// fails when the second entry is missing.
			return snapBuild(0, 2, snapEntry([]byte("a"), bytes.Repeat([]byte("v"), 91)))
		}},
		{"count_less_than_entries_present", func() []byte {
			return snapBuild(0, 1, snapEntry([]byte("a"), []byte("1")), snapEntry([]byte("b"), []byte("2")))
		}},

		{"zero_key_length", func() []byte { return snapBuild(0, 1, snapEntry(nil, nil), []byte{0}) }},
		{"key_length_over_limit", func() []byte {
			e := make([]byte, 8)
			binary.BigEndian.PutUint32(e[0:4], MaxKeySize+1)
			return snapBuild(0, 1, e, []byte{0})
		}},
		{"value_length_over_limit", func() []byte {
			e := make([]byte, 8)
			binary.BigEndian.PutUint32(e[0:4], 1)
			binary.BigEndian.PutUint32(e[4:8], MaxValueSize+1)
			return snapBuild(0, 1, e, []byte("k"))
		}},
		{"entry_exceeds_remaining_bytes", func() []byte {
			e := make([]byte, 8)
			binary.BigEndian.PutUint32(e[0:4], 10)  // claims 10-byte key
			return snapBuild(0, 1, e, []byte("ab")) // only 2 bytes follow
		}},

		{"duplicate_key", func() []byte {
			return snapBuild(0, 2, snapEntry([]byte("a"), []byte("1")), snapEntry([]byte("a"), []byte("2")))
		}},
		{"trailing_bytes_after_last_entry", func() []byte {
			return snapBuild(0, 1, snapEntry([]byte("a"), []byte("b")), []byte{1, 2, 3})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			snapPut(t, dir, tt.build())
			m, _, found, err := LoadSnapshot(dir)
			if !errors.Is(err, ErrBadSnapshot) {
				t.Fatalf("err = %v, want ErrBadSnapshot", err)
			}
			if found || m != nil {
				t.Fatalf("a corrupt snapshot must return nothing: m=%v found=%v", m, found)
			}
		})
	}
}

func TestLoadSnapshotRejectsBadChecksum(t *testing.T) {
	t.Run("footer_flipped", func(t *testing.T) {
		b := snapBuild(5, 1, snapEntry([]byte("a"), []byte("b")))
		b[len(b)-1] ^= 0xFF
		dir := t.TempDir()
		snapPut(t, dir, b)
		if _, _, _, err := LoadSnapshot(dir); !errors.Is(err, ErrBadSnapshot) {
			t.Fatalf("err = %v, want ErrBadSnapshot", err)
		}
	})
	t.Run("value_byte_flipped_crc_not_fixed", func(t *testing.T) {
		b := snapBuild(5, 1, snapEntry([]byte("a"), []byte("b")))
		b[len(b)-snapFooterSize-1] ^= 0x01 // the value byte; structure still valid
		dir := t.TempDir()
		snapPut(t, dir, b)
		if _, _, _, err := LoadSnapshot(dir); !errors.Is(err, ErrBadSnapshot) {
			t.Fatalf("err = %v, want ErrBadSnapshot", err)
		}
	})
	t.Run("walOffset_flipped_crc_not_fixed", func(t *testing.T) {
		b := snapBuild(5, 1, snapEntry([]byte("a"), []byte("b")))
		b[19] ^= 0x01 // low byte of walOffset: the most dangerous silent corruption
		dir := t.TempDir()
		snapPut(t, dir, b)
		if _, _, _, err := LoadSnapshot(dir); !errors.Is(err, ErrBadSnapshot) {
			t.Fatalf("err = %v, want ErrBadSnapshot", err)
		}
	})
}

// ---------- corruption: exhaustive sweeps over a real snapshot ----------

func snapSample(t *testing.T) []byte {
	t.Helper()
	dir := t.TempDir()
	data := map[string][]byte{"alpha": []byte("1"), "beta": []byte("22"), "gamma": nil}
	if err := WriteSnapshot(dir, data, 77); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, SnapshotFile))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestLoadSnapshotRejectsEveryTruncation(t *testing.T) {
	good := snapSample(t)
	for n := 0; n < len(good); n++ { // every strict prefix, including empty
		dir := t.TempDir()
		snapPut(t, dir, good[:n])
		if _, _, found, err := LoadSnapshot(dir); !errors.Is(err, ErrBadSnapshot) || found {
			t.Fatalf("prefix len %d: found=%v err=%v, want ErrBadSnapshot", n, found, err)
		}
	}
}

func TestLoadSnapshotRejectsEveryFlippedByte(t *testing.T) {
	good := snapSample(t)
	for i := range good {
		b := append([]byte(nil), good...)
		b[i] ^= 0xFF
		dir := t.TempDir()
		snapPut(t, dir, b)
		if _, _, found, err := LoadSnapshot(dir); !errors.Is(err, ErrBadSnapshot) || found {
			t.Fatalf("byte %d flipped: found=%v err=%v, want ErrBadSnapshot", i, found, err)
		}
	}
}

func TestLoadSnapshotRejectsAppendedGarbage(t *testing.T) {
	good := snapSample(t)
	dir := t.TempDir()
	snapPut(t, dir, append(append([]byte(nil), good...), 0x00))
	if _, _, found, err := LoadSnapshot(dir); !errors.Is(err, ErrBadSnapshot) || found {
		t.Fatalf("found=%v err=%v, want ErrBadSnapshot", found, err)
	}
}
