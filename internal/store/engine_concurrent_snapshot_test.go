package store

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ashrjz/raftkv/internal/wal"
)

// mustFinish fails the test if fn does not return in time. fn must not call
// t.Fatal*, since it runs in its own goroutine.
func mustFinish(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { fn(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s blocked while the snapshot write was in progress", what)
	}
}

// stallSnapshotWrite makes the next snapshot file write block until release
// is closed. started is closed once the write has begun.
func stallSnapshotWrite(e *Engine) (started <-chan struct{}, release chan<- struct{}) {
	s := make(chan struct{})
	r := make(chan struct{})
	real := e.writeSnapshot
	var once sync.Once
	e.writeSnapshot = func(dir string, data map[string][]byte, off uint64) error {
		once.Do(func() { close(s) })
		<-r
		return real(dir, data, off)
	}
	return s, r
}

func TestWritesAndReadsProceedDuringSnapshotWrite(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)
	snapPut(t, e, "a", "1")

	started, release := stallSnapshotWrite(e)
	snapDone := make(chan error, 1)
	go func() { snapDone <- e.Snapshot() }()
	<-started // capture is done; the file write is now stalled

	var putErr, delErr, getErr error
	var got []byte
	mustFinish(t, "Put", func() { putErr = e.Put("b", []byte("2")) })
	mustFinish(t, "Delete", func() { delErr = e.Delete("a") })
	mustFinish(t, "Get", func() { got, getErr = e.Get("b") })
	if putErr != nil || delErr != nil || getErr != nil || string(got) != "2" {
		t.Fatalf("put=%v del=%v get=%q,%v", putErr, delErr, got, getErr)
	}

	close(release)
	if err := <-snapDone; err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// The snapshot is a point-in-time image of the capture instant: it has
	// "a" (deleted afterwards) and not "b" (written afterwards).
	snap, _, found, err := wal.LoadSnapshot(dir)
	if err != nil || !found {
		t.Fatalf("LoadSnapshot: found=%v err=%v", found, err)
	}
	if _, ok := snap["a"]; !ok {
		t.Fatal(`snapshot lost "a", which existed at the capture instant`)
	}
	if _, ok := snap["b"]; ok {
		t.Fatal(`snapshot contains "b", written after the capture instant`)
	}

	// The writes made during the snapshot are the WAL tail: truncation must
	// have kept them, so a restart sees the final state.
	if sz := truncWALSize(t, dir); sz <= wal.HeaderSize {
		t.Fatalf("WAL size = %d: the tail written during the snapshot was dropped", sz)
	}
	e.Close()

	e = snapOpen(t, dir)
	defer e.Close()
	snapAbsent(t, e, "a")
	snapWant(t, e, "b", "2")
	if e.recovery.Records != 2 {
		t.Fatalf("replayed %d records, want 2 (the Put and the Delete)", e.recovery.Records)
	}
}

func TestSnapshotWriteFailureDoesNotTruncate(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)
	defer e.Close()
	for i := 0; i < 10; i++ {
		snapPut(t, e, fmt.Sprintf("k%d", i), "v")
	}
	before := truncWALSize(t, dir)

	real := e.writeSnapshot
	boom := errors.New("disk full")
	e.writeSnapshot = func(string, map[string][]byte, uint64) error { return boom }
	if err := e.Snapshot(); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if sz := truncWALSize(t, dir); sz != before {
		t.Fatalf("WAL changed after a failed snapshot: %d -> %d", before, sz)
	}
	snapWant(t, e, "k0", "v") // engine still healthy

	// A later snapshot succeeds and truncates normally.
	e.writeSnapshot = real
	snapTake(t, e)
	if sz := truncWALSize(t, dir); sz != wal.HeaderSize {
		t.Fatalf("WAL size = %d after a good snapshot, want %d", sz, wal.HeaderSize)
	}
}

func TestConcurrentSnapshotsSerialize(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)

	var cur, max int32
	real := e.writeSnapshot
	e.writeSnapshot = func(d string, data map[string][]byte, off uint64) error {
		n := atomic.AddInt32(&cur, 1)
		for {
			m := atomic.LoadInt32(&max)
			if n <= m || atomic.CompareAndSwapInt32(&max, m, n) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond) // widen the overlap window
		err := real(d, data, off)
		atomic.AddInt32(&cur, -1)
		return err
	}

	const writers, perWriter, snappers, perSnapper = 4, 100, 6, 3
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if err := e.Put(fmt.Sprintf("w%d-k%d", w, i), []byte("x")); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	for s := 0; s < snappers; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perSnapper; i++ {
				if err := e.Snapshot(); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()

	if m := atomic.LoadInt32(&max); m != 1 {
		t.Fatalf("up to %d snapshot writes ran at once, want exactly 1", m)
	}
	e.Close()

	// If an older snapshot had replaced a newer one, startup would fail with
	// ErrBadOffset or lose keys.
	e = snapOpen(t, dir)
	defer e.Close()
	for w := 0; w < writers; w++ {
		for i := 0; i < perWriter; i++ {
			snapWant(t, e, fmt.Sprintf("w%d-k%d", w, i), "x")
		}
	}
}

func TestCloseWaitsForInFlightSnapshot(t *testing.T) {
	dir := t.TempDir()
	e := snapOpen(t, dir)
	snapPut(t, e, "a", "1")

	started, release := stallSnapshotWrite(e)
	snapDone := make(chan error, 1)
	go func() { snapDone <- e.Snapshot() }()
	<-started

	closed := make(chan error, 1)
	go func() { closed <- e.Close() }()

	select {
	case <-closed:
		t.Fatal("Close returned while a snapshot was still in flight")
	case <-time.After(100 * time.Millisecond):
		// A late Close can only make this check pass, never fail wrongly.
	}

	close(release)
	if err := <-snapDone; err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}

	e = snapOpen(t, dir)
	defer e.Close()
	snapWant(t, e, "a", "1")
}
