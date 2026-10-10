package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Ashrjz/raftkv/internal/wal"
)

func truncWALSize(t *testing.T, dir string) int64 {
	t.Helper()
	st, err := os.Stat(snapWalPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}

func TestSnapshotShrinksWALToHeader(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)
	for i := 0; i < 100; i++ {
		snapPut(t, e, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	before := truncWALSize(t, dir)

	snapTake(t, e)
	after := truncWALSize(t, dir)
	if after != wal.HeaderSize || after >= before {
		t.Fatalf("WAL size %d -> %d, want header-only (%d)", before, after, wal.HeaderSize)
	}
	e.Close()

	e = snapOpen(t, dir)
	defer e.Close()
	for i := 0; i < 100; i++ {
		snapWant(t, e, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	if e.recovery.Records != 0 {
		t.Fatalf("replayed %d records, want 0", e.recovery.Records)
	}
}

func TestWritesAfterTruncationSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)
	snapPut(t, e, "a", "1")
	snapPut(t, e, "b", "2")
	snapTake(t, e)

	snapPut(t, e, "a", "10")
	snapDel(t, e, "b")
	snapPut(t, e, "c", "3")
	e.Close()

	e = snapOpen(t, dir)
	defer e.Close()
	snapWant(t, e, "a", "10")
	snapAbsent(t, e, "b")
	snapWant(t, e, "c", "3")
	if e.recovery.Records != 3 {
		t.Fatalf("replayed %d records, want 3", e.recovery.Records)
	}
}

func TestRepeatedSnapshotCyclesStayBounded(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)

	for cycle := 0; cycle < 5; cycle++ {
		for i := 0; i < 50; i++ {
			snapPut(t, e, fmt.Sprintf("c%d-k%d", cycle, i), "v")
		}
		snapTake(t, e)
		if sz := truncWALSize(t, dir); sz != wal.HeaderSize {
			t.Fatalf("cycle %d: WAL size = %d, want %d", cycle, sz, wal.HeaderSize)
		}
	}
	snapPut(t, e, "tail", "t")
	e.Close()

	e = snapOpen(t, dir)
	defer e.Close()
	for cycle := 0; cycle < 5; cycle++ {
		for i := 0; i < 50; i++ {
			snapWant(t, e, fmt.Sprintf("c%d-k%d", cycle, i), "v")
		}
	}
	snapWant(t, e, "tail", "t")
	if e.recovery.Records != 1 {
		t.Fatalf("replayed %d records, want 1 (only after the last snapshot)", e.recovery.Records)
	}
}

// Simulates a crash after the snapshot is durable but before WAL truncation:
// the snapshot exists, the WAL is still the full untruncated file.
func TestCrashBetweenSnapshotAndTruncation(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)
	for i := 0; i < 20; i++ {
		snapPut(t, e, fmt.Sprintf("k%d", i), "v")
	}
	err := e.mem.withData(func(data map[string][]byte) error {
		return wal.WriteSnapshot(e.dir, data, e.wal.LogicalEnd()) // no truncate
	})
	if err != nil {
		t.Fatal(err)
	}
	fullSize := truncWALSize(t, dir)
	snapPut(t, e, "after", "x")
	e.Close()

	e = snapOpen(t, dir)
	for i := 0; i < 20; i++ {
		snapWant(t, e, fmt.Sprintf("k%d", i), "v")
	}
	snapWant(t, e, "after", "x")
	if e.recovery.Records != 1 {
		t.Fatalf("replayed %d records, want 1 (snapshot offset still honoured)", e.recovery.Records)
	}
	if truncWALSize(t, dir) <= fullSize {
		t.Fatal("WAL unexpectedly shrank without a truncation")
	}

	// The next Snapshot() cleans up.
	snapTake(t, e)
	if sz := truncWALSize(t, dir); sz != wal.HeaderSize {
		t.Fatalf("WAL size = %d after retry, want %d", sz, wal.HeaderSize)
	}
	e.Close()
}

// After truncation the WAL alone can no longer rebuild the state, so a bad
// snapshot must stop startup rather than silently lose data.
func TestCorruptSnapshotAfterTruncationFailsStartup(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)
	snapPut(t, e, "a", "1")
	snapTake(t, e)
	e.Close()

	p := filepath.Join(dir, wal.SnapshotFile)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 0xFF
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	e2, err := NewEngine(snapWalPath(dir))
	if !errors.Is(err, wal.ErrBadSnapshot) {
		if e2 != nil {
			e2.Close()
		}
		t.Fatalf("err = %v, want wal.ErrBadSnapshot", err)
	}
}

// A truncated WAL paired with a missing snapshot is unrecoverable and must
// be reported, not treated as an empty database.
func TestTruncatedWALWithoutSnapshotFailsStartup(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)
	snapPut(t, e, "a", "1")
	snapTake(t, e)
	e.Close()

	if err := os.Remove(filepath.Join(dir, wal.SnapshotFile)); err != nil {
		t.Fatal(err)
	}
	e2, err := NewEngine(snapWalPath(dir))
	if !errors.Is(err, wal.ErrBadOffset) {
		if e2 != nil {
			e2.Close()
		}
		t.Fatalf("err = %v, want wal.ErrBadOffset", err)
	}
}

func TestStaleWALTmpRemovedOnEngineStartup(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)
	snapPut(t, e, "a", "1")
	e.Close()

	tmp := snapWalPath(dir) + ".tmp"
	if err := os.WriteFile(tmp, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	e = snapOpen(t, dir)
	defer e.Close()
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("stale WAL tmp survived startup (stat err = %v)", err)
	}
	snapWant(t, e, "a", "1")
}
