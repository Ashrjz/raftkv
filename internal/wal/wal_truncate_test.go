package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func truncHeaderBase(t *testing.T, path string) uint64 {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	base, err := validateHeader(b[:HeaderSize])
	if err != nil {
		t.Fatal(err)
	}
	return base
}

func truncFileSize(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}

func TestTruncateBeforeEmptyTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	bounds := offBuild(t, path, 5)

	w, _, _, err := offRecover(t, path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.TruncateBefore(bounds[5]); err != nil {
		t.Fatal(err)
	}
	if got := w.LogicalEnd(); got != bounds[5] {
		t.Fatalf("LogicalEnd = %d, want %d (must not change)", got, bounds[5])
	}
	if sz := truncFileSize(t, path); sz != HeaderSize {
		t.Fatalf("file size = %d, want %d (header only)", sz, HeaderSize)
	}
	if base := truncHeaderBase(t, path); base != bounds[5] {
		t.Fatalf("BaseOffset = %d, want %d", base, bounds[5])
	}
	if _, err := os.Stat(path + walTmpSuffix); !os.IsNotExist(err) {
		t.Fatalf("tmp file left behind (stat err = %v)", err)
	}
	w.Close()

	w2, got, _, err := offRecover(t, path, bounds[5])
	if err != nil || len(got) != 0 {
		t.Fatalf("reopen: %d records, err=%v", len(got), err)
	}
	if w2.LogicalEnd() != bounds[5] {
		t.Fatalf("LogicalEnd after reopen = %d, want %d", w2.LogicalEnd(), bounds[5])
	}
}

func TestTruncateBeforeKeepsTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	bounds := offBuild(t, path, 5)

	w, _, _, err := offRecover(t, path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.TruncateBefore(bounds[2]); err != nil {
		t.Fatal(err)
	}
	if got := w.LogicalEnd(); got != bounds[5] {
		t.Fatalf("LogicalEnd = %d, want %d", got, bounds[5])
	}
	want := int64(HeaderSize) + int64(bounds[5]-bounds[2])
	if sz := truncFileSize(t, path); sz != want {
		t.Fatalf("file size = %d, want %d", sz, want)
	}
	w.Close()

	// The same logical offsets select the same records as before truncation.
	_, got, _, err := offRecover(t, path, bounds[2])
	if err != nil {
		t.Fatal(err)
	}
	offCheckRecords(t, got, 2, 5)

	_, got, _, err = offRecover(t, path, bounds[3])
	if err != nil {
		t.Fatal(err)
	}
	offCheckRecords(t, got, 3, 5)

	// Anything before the new base is gone and must be rejected.
	for _, from := range []uint64{0, bounds[1], bounds[2] - 1} {
		if _, _, _, err := offRecover(t, path, from); !errors.Is(err, ErrBadOffset) {
			t.Fatalf("from=%d: err = %v, want ErrBadOffset", from, err)
		}
	}
}

func TestTruncateBeforeNoOp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.TruncateBefore(0); err != nil { // fresh WAL, nothing to drop
		t.Fatal(err)
	}
	if err := w.Append(offPut(0)); err != nil {
		t.Fatal(err)
	}
	end := w.LogicalEnd()
	if err := w.TruncateBefore(end); err != nil {
		t.Fatal(err)
	}
	if err := w.TruncateBefore(end); err != nil { // already at base
		t.Fatal(err)
	}
	if _, err := os.Stat(path + walTmpSuffix); !os.IsNotExist(err) {
		t.Fatalf("tmp file left behind (stat err = %v)", err)
	}
}

func TestTruncateBeforeRejectsBadRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	bounds := offBuild(t, path, 4)

	w, _, _, err := offRecover(t, path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.TruncateBefore(bounds[2]); err != nil {
		t.Fatal(err)
	}
	before := truncFileSize(t, path)

	for _, upTo := range []uint64{0, bounds[1], bounds[4] + 1} {
		if err := w.TruncateBefore(upTo); !errors.Is(err, ErrBadOffset) {
			t.Fatalf("upTo=%d: err = %v, want ErrBadOffset", upTo, err)
		}
	}
	if sz := truncFileSize(t, path); sz != before {
		t.Fatalf("file changed on a rejected truncate: %d -> %d", before, sz)
	}
	// The WAL is still healthy.
	if err := w.Append(offPut(9)); err != nil {
		t.Fatalf("append after rejected truncate: %v", err)
	}
}

func TestAppendAfterTruncateGoesToNewFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	bounds := offBuild(t, path, 3)

	w, _, _, err := offRecover(t, path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.TruncateBefore(bounds[3]); err != nil {
		t.Fatal(err)
	}
	// If the handle still pointed at the unlinked old file, these appends
	// would "succeed" and vanish on reopen.
	for i := 3; i < 6; i++ {
		if err := w.Append(offPut(i)); err != nil {
			t.Fatal(err)
		}
	}
	end := w.LogicalEnd()
	w.Close()

	w2, got, _, err := offRecover(t, path, bounds[3])
	if err != nil {
		t.Fatal(err)
	}
	offCheckRecords(t, got, 3, 6)
	if w2.LogicalEnd() != end {
		t.Fatalf("LogicalEnd after reopen = %d, want %d", w2.LogicalEnd(), end)
	}
}

func TestRepeatedTruncationCyclesStayBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { w.Close() }()

	var lastCut uint64
	total := 0
	for cycle := 0; cycle < 10; cycle++ {
		for i := 0; i < 3; i++ {
			if err := w.Append(offPut(total)); err != nil {
				t.Fatal(err)
			}
			total++
		}
		lastCut = w.LogicalEnd()
		if err := w.TruncateBefore(lastCut); err != nil {
			t.Fatal(err)
		}
		if sz := truncFileSize(t, path); sz != HeaderSize {
			t.Fatalf("cycle %d: file size = %d, want %d", cycle, sz, HeaderSize)
		}
	}
	if err := w.Append(offPut(total)); err != nil {
		t.Fatal(err)
	}
	total++
	w.Close()

	_, got, _, err := offRecover(t, path, lastCut)
	if err != nil {
		t.Fatal(err)
	}
	offCheckRecords(t, got, total-1, total)
}

func TestTruncateThenTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	bounds := offBuild(t, path, 4)

	w, _, _, err := offRecover(t, path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.TruncateBefore(bounds[2]); err != nil {
		t.Fatal(err)
	}
	w.Close()

	offAppendRaw(t, path, []byte{0x01, 0x02})

	w2, got, res, err := offRecover(t, path, bounds[2])
	if err != nil {
		t.Fatal(err)
	}
	offCheckRecords(t, got, 2, 4)
	if res.DiscardedBytes != 2 || w2.LogicalEnd() != bounds[4] {
		t.Fatalf("res=%+v LogicalEnd=%d, want discard 2 / end %d", res, w2.LogicalEnd(), bounds[4])
	}
}

func TestOpenRemovesStaleTruncateTmp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	bounds := offBuild(t, path, 2)

	if err := os.WriteFile(path+walTmpSuffix, []byte("half-written"), 0o644); err != nil {
		t.Fatal(err)
	}
	w, got, _, err := offRecover(t, path, 0)
	if err != nil {
		t.Fatal(err)
	}
	offCheckRecords(t, got, 0, 2)
	if w.LogicalEnd() != bounds[2] {
		t.Fatalf("LogicalEnd = %d, want %d", w.LogicalEnd(), bounds[2])
	}
	if _, err := os.Stat(path + walTmpSuffix); !os.IsNotExist(err) {
		t.Fatalf("stale tmp survived Open (stat err = %v)", err)
	}
}

func TestTruncateBeforeOnBrokenWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	bounds := offBuild(t, path, 2)
	w, _, _, err := offRecover(t, path, 0)
	if err != nil {
		t.Fatal(err)
	}
	w.broken = ErrBroken
	if err := w.TruncateBefore(bounds[2]); !errors.Is(err, ErrBroken) {
		t.Fatalf("err = %v, want ErrBroken", err)
	}
}
