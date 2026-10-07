package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// ---------- helpers ----------

func offPut(i int) Record {
	return Record{
		Type:  TypePut,
		Key:   []byte(fmt.Sprintf("key-%d", i)),
		Value: []byte(fmt.Sprintf("val-%d", i)),
	}
}

func offSize(t *testing.T, r Record) uint64 {
	t.Helper()
	b, err := encodeRecord(r)
	if err != nil {
		t.Fatal(err)
	}
	return uint64(len(b))
}

// offBuild creates a fresh WAL at path with n records and returns the logical
// offset at every record boundary: bounds[0] = 0, bounds[i] = end of record i-1.
func offBuild(t *testing.T, path string, n int) []uint64 {
	t.Helper()
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	bounds := []uint64{w.LogicalEnd()}
	for i := 0; i < n; i++ {
		if err := w.Append(offPut(i)); err != nil {
			t.Fatal(err)
		}
		bounds = append(bounds, w.LogicalEnd())
	}
	return bounds
}

// offRecover opens path and recovers from `from`, collecting applied records.
// The WAL is closed on test cleanup.
func offRecover(t *testing.T, path string, from uint64) (*WAL, []Record, RecoveryResult, error) {
	t.Helper()
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	var got []Record
	res, err := w.Recover(from, func(r Record) error {
		got = append(got, r)
		return nil
	})
	return w, got, res, err
}

// offWriteFile writes a WAL file by hand with an arbitrary BaseOffset and
// returns the logical end. Used to simulate a WAL after truncation.
func offWriteFile(t *testing.T, path string, base uint64, recs ...Record) uint64 {
	t.Helper()
	buf := encodeHeader(base)
	end := base
	for _, r := range recs {
		b, err := encodeRecord(r)
		if err != nil {
			t.Fatal(err)
		}
		buf = append(buf, b...)
		end += uint64(len(b))
	}
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	return end
}

func offAppendRaw(t *testing.T, path string, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
}

func offCheckRecords(t *testing.T, got []Record, from, to int) {
	t.Helper()
	if len(got) != to-from {
		t.Fatalf("got %d records, want %d", len(got), to-from)
	}
	for i, r := range got {
		want := offPut(from + i)
		if r.Type != want.Type || string(r.Key) != string(want.Key) || string(r.Value) != string(want.Value) {
			t.Fatalf("record %d = %+v, want %+v", i, r, want)
		}
	}
}

// ---------- logical end tracking ----------

func TestLogicalEndTracksAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if got := w.LogicalEnd(); got != 0 {
		t.Fatalf("new WAL LogicalEnd = %d, want 0", got)
	}
	var want uint64
	for i := 0; i < 5; i++ {
		r := offPut(i)
		if err := w.Append(r); err != nil {
			t.Fatal(err)
		}
		want += offSize(t, r)
		if got := w.LogicalEnd(); got != want {
			t.Fatalf("after append %d: LogicalEnd = %d, want %d", i, got, want)
		}
	}
}

func TestLogicalEndSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	bounds := offBuild(t, path, 4)

	w, _, _, err := offRecover(t, path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := w.LogicalEnd(); got != bounds[4] {
		t.Fatalf("LogicalEnd after reopen = %d, want %d", got, bounds[4])
	}
}

func TestLogicalEndDoesNotAdvanceOnRejectedAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Append(offPut(0)); err != nil {
		t.Fatal(err)
	}
	before := w.LogicalEnd()
	if err := w.Append(Record{Type: TypePut}); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("err = %v, want ErrEmptyKey", err)
	}
	if got := w.LogicalEnd(); got != before {
		t.Fatalf("LogicalEnd moved on a rejected record: %d -> %d", before, got)
	}
}

// ---------- Recover(from) ----------

func TestRecoverFromEveryBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	const n = 5
	bounds := offBuild(t, path, n)

	for i := 0; i <= n; i++ { // i == n means "exactly at the end": apply nothing
		t.Run(fmt.Sprintf("from_record_%d", i), func(t *testing.T) {
			w, got, res, err := offRecover(t, path, bounds[i])
			if err != nil {
				t.Fatal(err)
			}
			offCheckRecords(t, got, i, n)
			if res.Records != n-i || res.DiscardedBytes != 0 {
				t.Fatalf("res = %+v", res)
			}
			if w.LogicalEnd() != bounds[n] {
				t.Fatalf("LogicalEnd = %d, want %d", w.LogicalEnd(), bounds[n])
			}
		})
	}
}

func TestRecoverFromBeyondEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	bounds := offBuild(t, path, 3)

	_, got, _, err := offRecover(t, path, bounds[3]+1)
	if !errors.Is(err, ErrBadOffset) {
		t.Fatalf("err = %v, want ErrBadOffset", err)
	}
	if len(got) != 0 {
		t.Fatalf("applied %d records on a rejected offset", len(got))
	}
}

func TestRecoverWithNonZeroBaseOffset(t *testing.T) {
	const base = 256
	r0, r1, r2 := offPut(0), offPut(1), offPut(2)
	path := filepath.Join(t.TempDir(), "wal.log")
	end := offWriteFile(t, path, base, r0, r1, r2)
	b1 := base + offSize(t, r0)
	b2 := b1 + offSize(t, r1)

	t.Run("from_base_replays_all", func(t *testing.T) {
		w, got, _, err := offRecover(t, path, base)
		if err != nil {
			t.Fatal(err)
		}
		offCheckRecords(t, got, 0, 3)
		if w.LogicalEnd() != end {
			t.Fatalf("LogicalEnd = %d, want %d", w.LogicalEnd(), end)
		}
	})
	t.Run("from_mid_boundary", func(t *testing.T) {
		_, got, _, err := offRecover(t, path, b2)
		if err != nil {
			t.Fatal(err)
		}
		offCheckRecords(t, got, 2, 3)
	})
	t.Run("from_end", func(t *testing.T) {
		_, got, _, err := offRecover(t, path, end)
		if err != nil || len(got) != 0 {
			t.Fatalf("got %d records, err=%v", len(got), err)
		}
	})
	t.Run("below_base_is_rejected", func(t *testing.T) {
		for _, from := range []uint64{0, base - 1} {
			_, got, _, err := offRecover(t, path, from)
			if !errors.Is(err, ErrBadOffset) {
				t.Fatalf("from=%d: err = %v, want ErrBadOffset", from, err)
			}
			if len(got) != 0 {
				t.Fatalf("from=%d: applied %d records", from, len(got))
			}
		}
	})
	t.Run("above_end_is_rejected", func(t *testing.T) {
		_, _, _, err := offRecover(t, path, end+1)
		if !errors.Is(err, ErrBadOffset) {
			t.Fatalf("err = %v, want ErrBadOffset", err)
		}
	})
	t.Run("same_logical_offsets_as_untruncated_log", func(t *testing.T) {
		// A snapshot taken before truncation stores b1; after truncation the
		// same logical offset must still select the same record.
		_, got, _, err := offRecover(t, path, b1)
		if err != nil {
			t.Fatal(err)
		}
		offCheckRecords(t, got, 1, 3)
	})
}

func TestRecoverEmptyWALWithNonZeroBase(t *testing.T) {
	const base = 256
	path := filepath.Join(t.TempDir(), "wal.log")
	offWriteFile(t, path, base) // header only

	w, got, _, err := offRecover(t, path, base)
	if err != nil || len(got) != 0 {
		t.Fatalf("got %d records, err=%v", len(got), err)
	}
	if w.LogicalEnd() != base {
		t.Fatalf("LogicalEnd = %d, want %d", w.LogicalEnd(), base)
	}
	if _, _, _, err := offRecover(t, path, base-1); !errors.Is(err, ErrBadOffset) {
		t.Fatalf("err = %v, want ErrBadOffset", err)
	}
}

func TestAppendAfterNonZeroBaseAdvancesLogicalEnd(t *testing.T) {
	const base = 1000
	path := filepath.Join(t.TempDir(), "wal.log")
	end := offWriteFile(t, path, base, offPut(0))

	w, _, _, err := offRecover(t, path, base)
	if err != nil {
		t.Fatal(err)
	}
	r := offPut(1)
	if err := w.Append(r); err != nil {
		t.Fatal(err)
	}
	if want := end + offSize(t, r); w.LogicalEnd() != want {
		t.Fatalf("LogicalEnd = %d, want %d", w.LogicalEnd(), want)
	}
	w.Close()

	_, got, _, err := offRecover(t, path, base)
	if err != nil {
		t.Fatal(err)
	}
	offCheckRecords(t, got, 0, 2)
}

// ---------- torn tail interactions ----------

func TestTornTailExcludedFromLogicalEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	bounds := offBuild(t, path, 3)

	torn, err := encodeRecord(offPut(99))
	if err != nil {
		t.Fatal(err)
	}
	torn = torn[:len(torn)/2]
	offAppendRaw(t, path, torn)

	w, got, res, err := offRecover(t, path, 0)
	if err != nil {
		t.Fatal(err)
	}
	offCheckRecords(t, got, 0, 3)
	if res.DiscardedBytes != int64(len(torn)) {
		t.Fatalf("DiscardedBytes = %d, want %d", res.DiscardedBytes, len(torn))
	}
	if w.LogicalEnd() != bounds[3] {
		t.Fatalf("LogicalEnd = %d, want %d (torn bytes must not count)", w.LogicalEnd(), bounds[3])
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(HeaderSize) + int64(bounds[3]); st.Size() != want {
		t.Fatalf("file size = %d, want %d after truncation", st.Size(), want)
	}

	// The WAL is usable and contiguous after repair.
	r := offPut(3)
	if err := w.Append(r); err != nil {
		t.Fatal(err)
	}
	if want := bounds[3] + offSize(t, r); w.LogicalEnd() != want {
		t.Fatalf("LogicalEnd after append = %d, want %d", w.LogicalEnd(), want)
	}
	w.Close()
	_, got, _, err = offRecover(t, path, 0)
	if err != nil {
		t.Fatal(err)
	}
	offCheckRecords(t, got, 0, 4)
}

func TestRecoverFromMidLogWithTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	bounds := offBuild(t, path, 4)
	offAppendRaw(t, path, []byte{0x00, 0x01, 0x02}) // < 9 bytes: torn prefix

	w, got, res, err := offRecover(t, path, bounds[2])
	if err != nil {
		t.Fatal(err)
	}
	offCheckRecords(t, got, 2, 4)
	if res.DiscardedBytes != 3 {
		t.Fatalf("DiscardedBytes = %d, want 3", res.DiscardedBytes)
	}
	if w.LogicalEnd() != bounds[4] {
		t.Fatalf("LogicalEnd = %d, want %d", w.LogicalEnd(), bounds[4])
	}
}

func TestRecoverFromAtEndWithTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	bounds := offBuild(t, path, 2)
	offAppendRaw(t, path, []byte{0xAA, 0xBB})

	w, got, res, err := offRecover(t, path, bounds[2])
	if err != nil || len(got) != 0 {
		t.Fatalf("got %d records, err=%v", len(got), err)
	}
	if res.DiscardedBytes != 2 || w.LogicalEnd() != bounds[2] {
		t.Fatalf("res=%+v LogicalEnd=%d", res, w.LogicalEnd())
	}
}

func TestTornHeaderOnlyAllowsFromZero(t *testing.T) {
	dir := t.TempDir()

	// from != 0 cannot be satisfied by a WAL with no header at all.
	p1 := filepath.Join(dir, "a.log")
	if err := os.WriteFile(p1, encodeHeader(0)[:7], 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := offRecover(t, p1, 5); !errors.Is(err, ErrBadOffset) {
		t.Fatalf("err = %v, want ErrBadOffset", err)
	}

	// from == 0 repairs it and the logical end is 0.
	p2 := filepath.Join(dir, "b.log")
	if err := os.WriteFile(p2, encodeHeader(0)[:7], 0o644); err != nil {
		t.Fatal(err)
	}
	w, got, res, err := offRecover(t, p2, 0)
	if err != nil || len(got) != 0 {
		t.Fatalf("got %d records, err=%v", len(got), err)
	}
	if res.DiscardReason != "torn header" || w.LogicalEnd() != 0 {
		t.Fatalf("res=%+v LogicalEnd=%d", res, w.LogicalEnd())
	}
}

func TestRecoverRejectsCorruptHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	offBuild(t, path, 2)

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[10] ^= 0xFF // inside BaseOffset
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := offRecover(t, path, 0); !errors.Is(err, ErrBadHeader) {
		t.Fatalf("err = %v, want ErrBadHeader", err)
	}
}
