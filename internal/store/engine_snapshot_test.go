package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/Ashrjz/raftkv/internal/wal"
)

// ---------- helpers ----------

func snapWalPath(dir string) string { return filepath.Join(dir, "wal.log") }

func snapOpen(t *testing.T, dir string) *Engine {
	t.Helper()
	e, err := NewEngine(snapWalPath(dir))
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	return e
}

func snapPut(t *testing.T, e *Engine, k, v string) {
	t.Helper()
	if err := e.Put(k, []byte(v)); err != nil {
		t.Fatalf("Put(%q): %v", k, err)
	}
}

func snapDel(t *testing.T, e *Engine, k string) {
	t.Helper()
	if err := e.Delete(k); err != nil {
		t.Fatalf("Delete(%q): %v", k, err)
	}
}

func snapTake(t *testing.T, e *Engine) {
	t.Helper()
	if err := e.Snapshot(); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
}

func snapWant(t *testing.T, e *Engine, k, v string) {
	t.Helper()
	got, err := e.Get(k)
	if err != nil || string(got) != v {
		t.Fatalf("Get(%q) = %q, %v; want %q", k, got, err, v)
	}
}

func snapAbsent(t *testing.T, e *Engine, k string) {
	t.Helper()
	if got, err := e.Get(k); err == nil {
		t.Fatalf("Get(%q) = %q, nil; want key absent", k, got)
	}
}

// ---------- recovery ----------

func TestRecoveryWithoutSnapshotReplaysWholeWAL(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)
	snapPut(t, e, "a", "1")
	snapPut(t, e, "b", "2")
	snapDel(t, e, "a")
	e.Close()

	e = snapOpen(t, dir)
	defer e.Close()
	snapAbsent(t, e, "a")
	snapWant(t, e, "b", "2")
	if e.recovery.Records != 3 {
		t.Fatalf("replayed %d records, want 3", e.recovery.Records)
	}
}

func TestSnapshotThenReopenRestoresState(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)

	snapPut(t, e, "a", "1")
	snapPut(t, e, "b", "2")
	snapPut(t, e, "c", "3")
	snapDel(t, e, "b")
	snapTake(t, e)

	// Four operations after the snapshot: overwrite, new key, delete of a
	// snapshotted key, another new key.
	snapPut(t, e, "a", "10")
	snapPut(t, e, "d", "4")
	snapDel(t, e, "c")
	snapPut(t, e, "e", "5")
	e.Close()

	e = snapOpen(t, dir)
	defer e.Close()
	snapWant(t, e, "a", "10")
	snapAbsent(t, e, "b") // deleted before the snapshot
	snapAbsent(t, e, "c") // deleted after the snapshot, via the WAL tail
	snapWant(t, e, "d", "4")
	snapWant(t, e, "e", "5")

	// Recovery cost scales with the WAL since the snapshot, not total history.
	if e.recovery.Records != 4 {
		t.Fatalf("replayed %d records, want 4 (only the tail)", e.recovery.Records)
	}
}

func TestReopenWithSnapshotAndEmptyTail(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)
	snapPut(t, e, "a", "1")
	snapPut(t, e, "b", "2")
	snapTake(t, e)
	e.Close()

	e = snapOpen(t, dir)
	defer e.Close()
	snapWant(t, e, "a", "1")
	snapWant(t, e, "b", "2")
	if e.recovery.Records != 0 {
		t.Fatalf("replayed %d records, want 0", e.recovery.Records)
	}
}

func TestSnapshotOfEmptyEngine(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)
	snapTake(t, e)
	snapPut(t, e, "a", "1")
	e.Close()

	e = snapOpen(t, dir)
	defer e.Close()
	snapWant(t, e, "a", "1")
	if e.recovery.Records != 1 {
		t.Fatalf("replayed %d records, want 1", e.recovery.Records)
	}
}

func TestLatestSnapshotWins(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)
	snapPut(t, e, "a", "1")
	snapTake(t, e)
	snapPut(t, e, "b", "2")
	snapTake(t, e)
	snapPut(t, e, "c", "3")
	e.Close()

	e = snapOpen(t, dir)
	defer e.Close()
	snapWant(t, e, "a", "1")
	snapWant(t, e, "b", "2")
	snapWant(t, e, "c", "3")
	if e.recovery.Records != 1 {
		t.Fatalf("replayed %d records, want 1 (only after the second snapshot)", e.recovery.Records)
	}
}

func TestSnapshotDoesNotChangeLiveState(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)
	defer e.Close()
	snapPut(t, e, "a", "1")
	snapTake(t, e)
	snapWant(t, e, "a", "1")
	snapPut(t, e, "a", "2") // writes still work after a snapshot
	snapWant(t, e, "a", "2")
}

func TestTornTailAfterSnapshotIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)
	snapPut(t, e, "a", "1")
	snapTake(t, e)
	snapPut(t, e, "b", "2")
	e.Close()

	f, err := os.OpenFile(snapWalPath(dir), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x00, 0x01, 0x02}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	e = snapOpen(t, dir)
	defer e.Close()
	snapWant(t, e, "a", "1")
	snapWant(t, e, "b", "2")
	if e.recovery.Records != 1 || e.recovery.DiscardedBytes != 3 {
		t.Fatalf("recovery = %+v, want Records=1 DiscardedBytes=3", e.recovery)
	}

	// Writes after repair land at the right place and survive another reopen.
	snapPut(t, e, "c", "3")
	e.Close()
	e = snapOpen(t, dir)
	defer e.Close()
	snapWant(t, e, "c", "3")
}

// ---------- startup failure and cleanup ----------

func TestCorruptSnapshotFailsStartup(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)
	snapPut(t, e, "a", "1")
	snapTake(t, e)
	snapPut(t, e, "b", "2")
	e.Close()

	p := filepath.Join(dir, wal.SnapshotFile)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)/2] ^= 0xFF
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}

	// The WAL alone could rebuild "b" but not "a"'s history; the engine must
	// refuse to start rather than silently fall back.
	e2, err := NewEngine(snapWalPath(dir))
	if !errors.Is(err, wal.ErrBadSnapshot) {
		if e2 != nil {
			e2.Close()
		}
		t.Fatalf("err = %v, want wal.ErrBadSnapshot", err)
	}
}

func TestMissingWALWithSnapshotFailsStartup(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)
	snapPut(t, e, "a", "1")
	snapTake(t, e) // snapshot covers WAL offset > 0
	e.Close()

	if err := os.Remove(snapWalPath(dir)); err != nil {
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

func TestStaleSnapshotTmpRemovedOnStartup(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)
	snapPut(t, e, "a", "1")
	e.Close()

	tmp := filepath.Join(dir, wal.SnapshotTmpFile)
	if err := os.WriteFile(tmp, []byte("half-written garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	e = snapOpen(t, dir)
	defer e.Close()
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("snapshot.tmp still present after startup (stat err = %v)", err)
	}
	snapWant(t, e, "a", "1")
}

// ---------- concurrency (run with -race) ----------

func TestSnapshotConcurrentWithWritesAndReads(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)

	const writers, perWriter, snapshots = 4, 300, 15

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				// "last" is overwritten in order; the final value must be perWriter-1.
				if err := e.Put(fmt.Sprintf("w%d-last", w), []byte(strconv.Itoa(i))); err != nil {
					t.Error(err)
					return
				}
				if err := e.Put(fmt.Sprintf("w%d-k%d", w, i), []byte("x")); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}

	stop := make(chan struct{})
	var bg sync.WaitGroup
	bg.Add(2)
	go func() { // snapshots while writes are in flight
		defer bg.Done()
		for i := 0; i < snapshots; i++ {
			if err := e.Snapshot(); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	go func() { // reads must keep working during snapshots
		defer bg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				e.Get("w0-last")
			}
		}
	}()

	wg.Wait()
	close(stop)
	bg.Wait()
	e.Close()

	// A snapshot whose map and WAL offset were captured at different instants
	// would lose or resurrect writes here.
	e = snapOpen(t, dir)
	defer e.Close()
	for w := 0; w < writers; w++ {
		snapWant(t, e, fmt.Sprintf("w%d-last", w), strconv.Itoa(perWriter-1))
		for i := 0; i < perWriter; i++ {
			snapWant(t, e, fmt.Sprintf("w%d-k%d", w, i), "x")
		}
	}
}
