package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func buildLog(t *testing.T, recs ...Record) (data []byte, ends []int) {
	t.Helper()
	data = encodeHeader()
	for _, r := range recs {
		b, err := encodeRecord(r)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, b...)
		ends = append(ends, len(data))
	}
	return data, ends
}

func writeFile(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "x.wal") // registered first => cleaned up last
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// recoverAt opens path, runs Recover, and leaves the WAL open for the caller.
func recoverAt(t *testing.T, path string) (*WAL, RecoveryResult, []Record, error) {
	t.Helper()
	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() }) // LIFO: runs before TempDir removal
	var got []Record
	res, err := w.Recover(func(r Record) error { got = append(got, r); return nil })
	return w, res, got, err
}

func put(k, v string) Record { return Record{Type: TypePut, Key: []byte(k), Value: []byte(v)} }

func keys(rs []Record) string {
	var s string
	for _, r := range rs {
		s += string(r.Key)
	}
	return s
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}

func TestRecoverCleanLog(t *testing.T) {
	data, _ := buildLog(t, put("a", "1"), put("b", "2"))
	_, res, got, err := recoverAt(t, writeFile(t, data))
	if err != nil || keys(got) != "ab" || res.DiscardedBytes != 0 {
		t.Fatalf("res=%+v got=%s err=%v", res, keys(got), err)
	}
}

// Every possible crash point inside the last record must recover to the same
// state, leave a clean file, and accept (and later replay) new writes.
func TestRecoverTornAtEveryCutPoint(t *testing.T) {
	last := Record{Type: TypePut, Key: []byte("c"), Value: bytes.Repeat([]byte("z"), 50)}
	data, ends := buildLog(t, put("a", "1"), put("b", "2"), last)
	lastStart := ends[1]

	for cut := lastStart + 1; cut < len(data); cut++ {
		t.Run(fmt.Sprintf("cut=%d", cut), func(t *testing.T) {
			path := writeFile(t, data[:cut])
			w, res, got, err := recoverAt(t, path)
			if err != nil {
				t.Fatalf("recover: %v", err)
			}
			if keys(got) != "ab" {
				t.Fatalf("got %s want ab", keys(got))
			}
			if res.GoodOffset != int64(lastStart) || res.DiscardedBytes != int64(cut-lastStart) {
				t.Fatalf("res=%+v", res)
			}
			if fileSize(t, path) != int64(lastStart) {
				t.Fatalf("file not truncated: %d", fileSize(t, path))
			}
			if err := w.Append(put("d", "4")); err != nil {
				t.Fatalf("append after recovery: %v", err)
			}
			w.Close()

			_, res2, got2, err := recoverAt(t, path)
			if err != nil || keys(got2) != "abd" || res2.DiscardedBytes != 0 {
				t.Fatalf("second recovery: res=%+v got=%s err=%v", res2, keys(got2), err)
			}
		})
	}
}

func TestRecoverLastRecordBadCRCIsDiscarded(t *testing.T) {
	data, _ := buildLog(t, put("a", "1"), put("b", "2"), put("c", "3"))
	data[len(data)-1] ^= 0xFF // flip a payload byte of the LAST record
	_, res, got, err := recoverAt(t, writeFile(t, data))
	if err != nil || keys(got) != "ab" || res.DiscardedBytes == 0 {
		t.Fatalf("res=%+v got=%s err=%v", res, keys(got), err)
	}
}

func TestRecoverMidLogCorruptionIsFatalAndUntouched(t *testing.T) {
	data, ends := buildLog(t, put("a", "1"), put("b", "2"), put("c", "3"))
	data[ends[0]+12] ^= 0xFF // damage record 2; record 3 still follows
	path := writeFile(t, data)
	_, _, _, err := recoverAt(t, path)
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("want ErrChecksum, got %v", err)
	}
	if fileSize(t, path) != int64(len(data)) {
		t.Fatal("fatal corruption must not modify the file")
	}
}

func TestRecoverZeroFilledTail(t *testing.T) {
	data, _ := buildLog(t, put("a", "1"), put("b", "2"))
	data = append(data, make([]byte, 4096)...)
	path := writeFile(t, data)
	_, res, got, err := recoverAt(t, path)
	if err != nil || keys(got) != "ab" || res.DiscardedBytes != 4096 {
		t.Fatalf("res=%+v got=%s err=%v", res, keys(got), err)
	}
}

func TestRecoverNonZeroGarbageTailIsFatal(t *testing.T) {
	data, _ := buildLog(t, put("a", "1"))
	data = append(data, bytes.Repeat([]byte{0xFF}, 20)...)
	_, _, _, err := recoverAt(t, writeFile(t, data))
	if !errors.Is(err, ErrBadLength) {
		t.Fatalf("want ErrBadLength, got %v", err)
	}
}

// A valid checksum means the bytes were written intact, so this is never a crash artifact.
func TestRecoverValidCRCBadTypeIsFatal(t *testing.T) {
	data, _ := buildLog(t, put("a", "1"))
	payload := []byte{0, 0, 0, 1, 'x'}
	raw := make([]byte, 9+len(payload))
	binary.BigEndian.PutUint32(raw[4:8], uint32(len(payload)))
	raw[8] = 9 // unknown type
	copy(raw[9:], payload)
	binary.BigEndian.PutUint32(raw[0:4], crc32.Checksum(raw[4:], castagnoli))
	_, _, _, err := recoverAt(t, writeFile(t, append(data, raw...)))
	if !errors.Is(err, ErrBadType) {
		t.Fatalf("want ErrBadType, got %v", err)
	}
}

func TestRecoverTornHeaderIsRepaired(t *testing.T) {
	path := writeFile(t, encodeHeader()[:7])
	w, res, got, err := recoverAt(t, path)
	if err != nil || len(got) != 0 || res.DiscardReason != "torn header" {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if fileSize(t, path) != HeaderSize {
		t.Fatalf("size=%d", fileSize(t, path))
	}
	if err := w.Append(put("a", "1")); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverShortGarbageFileIsFatal(t *testing.T) {
	_, _, _, err := recoverAt(t, writeFile(t, []byte("garbage")))
	if !errors.Is(err, ErrBadHeader) {
		t.Fatalf("want ErrBadHeader, got %v", err)
	}
}

func TestRecoverBadMagicIsFatal(t *testing.T) {
	data, _ := buildLog(t, put("a", "1"))
	data[0] = 'X'
	_, _, _, err := recoverAt(t, writeFile(t, data))
	if !errors.Is(err, ErrBadHeader) {
		t.Fatalf("want ErrBadHeader, got %v", err)
	}
}
