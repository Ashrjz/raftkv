package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Ashrjz/raftkv/internal/wal"
)

// recBuildHistory writes `history` Put records over `keys` distinct keys
// straight into the WAL. Group commit and 32 writers make a large history
// cheap to build; the engine's own fsync-per-write path would take minutes.
func recBuildHistory(tb testing.TB, dir string, history, keys int) {
	tb.Helper()
	const goroutines = 32
	w, err := wal.Open(filepath.Join(dir, "wal.log"), wal.WithGroupCommit())
	if err != nil {
		tb.Fatal(err)
	}

	base, extra := history/goroutines, history%goroutines
	var wg sync.WaitGroup
	start := 0
	for g := 0; g < goroutines; g++ {
		count := base
		if g < extra {
			count++ // the first `extra` goroutines take one more record each
		}
		wg.Add(1)
		go func(start, count int) {
			defer wg.Done()
			for i := 0; i < count; i++ {
				n := start + i // unique across all goroutines: 0..history-1
				rec := wal.Record{
					Type:  wal.TypePut,
					Key:   []byte(fmt.Sprintf("key-%d", n%keys)),
					Value: []byte(fmt.Sprintf("value-%d", n)),
				}
				if err := w.Append(rec); err != nil {
					tb.Error(err)
					return
				}
			}
		}(start, count)
		start += count
	}
	wg.Wait()
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
}

func recOpen(tb testing.TB, dir string) *Engine {
	tb.Helper()
	e, err := NewEngine(filepath.Join(dir, "wal.log"))
	if err != nil {
		tb.Fatalf("NewEngine: %v", err)
	}
	return e
}

// recFixture builds a data dir holding `history` writes over `keys` keys.
// If snapshot is true, it snapshots (which truncates the WAL) and then adds
// exactly `tail` more writes on top. Tail writes overwrite key-0..key-(tail-1)
// with the value "tail", so the same fixture always has the same WAL layout.
func recFixture(tb testing.TB, history, keys, tail int, snapshot bool) string {
	tb.Helper()
	dir := tb.TempDir()
	recBuildHistory(tb, dir, history, keys)
	if !snapshot {
		return dir
	}
	e := recOpen(tb, dir) // replays the whole history to rebuild state
	if err := e.Snapshot(); err != nil {
		tb.Fatal(err)
	}
	for i := 0; i < tail; i++ {
		if err := e.Put(fmt.Sprintf("key-%d", i%keys), []byte("tail")); err != nil {
			tb.Fatal(err)
		}
	}
	if err := e.Close(); err != nil {
		tb.Fatal(err)
	}
	return dir
}

// timedOpen reopens the engine and reports how long recovery took.
func timedOpen(t *testing.T, dir string) (*Engine, time.Duration) {
	t.Helper()
	start := time.Now()
	e := recOpen(t, dir)
	return e, time.Since(start)
}

// The core claim: with the live state fixed, a 10x larger write history does
// not change what recovery has to replay or how big the WAL is.
func TestRecoveryReplaysOnlyTailNotHistory(t *testing.T) {
	if testing.Short() {
		t.Skip("builds large histories")
	}
	const keys, tail = 100, 50

	var sizes []int64
	for _, history := range []int{2_000, 20_000} {
		t.Run(fmt.Sprintf("history=%d", history), func(t *testing.T) {
			dir := recFixture(t, history, keys, tail, true)

			e, took := timedOpen(t, dir)
			defer e.Close()
			t.Logf("history=%d tail=%d: recovery took %v, replayed %d records",
				history, tail, took, e.recovery.Records)

			if e.recovery.Records != tail {
				t.Fatalf("replayed %d records, want %d (only the tail)", e.recovery.Records, tail)
			}

			// The recovered state must be right, not just fast.
			if got, err := e.Get("key-0"); err != nil || string(got) != "tail" {
				t.Fatalf("key-0 = %q, %v; want the tail value", got, err)
			}
			if got, err := e.Get("key-99"); err != nil || !strings.HasPrefix(string(got), "value-") {
				t.Fatalf("key-99 = %q, %v; want a value from the snapshotted history", got, err)
			}

			sizes = append(sizes, truncWALSize(t, dir))
		})
	}
	if len(sizes) == 2 && sizes[0] != sizes[1] {
		t.Fatalf("WAL size depends on history: %d vs %d bytes", sizes[0], sizes[1])
	}
}

// The other direction: with history fixed, the replay work grows with the tail.
func TestRecoveryWorkGrowsWithTail(t *testing.T) {
	if testing.Short() {
		t.Skip("builds large histories")
	}
	const history, keys = 2_000, 100

	var lastSize int64
	for _, tail := range []int{10, 100, 500} {
		t.Run(fmt.Sprintf("tail=%d", tail), func(t *testing.T) {
			dir := recFixture(t, history, keys, tail, true)

			e, took := timedOpen(t, dir)
			defer e.Close()
			t.Logf("history=%d tail=%d: recovery took %v, replayed %d records",
				history, tail, took, e.recovery.Records)

			if e.recovery.Records != tail {
				t.Fatalf("replayed %d records, want %d", e.recovery.Records, tail)
			}
			size := truncWALSize(t, dir)
			if size <= lastSize {
				t.Fatalf("WAL size %d did not grow with the tail (previous %d)", size, lastSize)
			}
			lastSize = size
		})
	}
}

// Control: the same history without a snapshot has to replay everything, and
// a snapshot with no tail replays nothing from a header-only WAL.
func TestRecoveryControlWithAndWithoutSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("builds large histories")
	}
	const history, keys = 5_000, 100

	t.Run("no_snapshot_replays_everything", func(t *testing.T) {
		dir := recFixture(t, history, keys, 0, false)
		e, took := timedOpen(t, dir)
		defer e.Close()
		t.Logf("no snapshot: recovery took %v, replayed %d records", took, e.recovery.Records)
		if e.recovery.Records != history {
			t.Fatalf("replayed %d records, want the whole history (%d)", e.recovery.Records, history)
		}
	})

	t.Run("snapshot_no_tail_replays_nothing", func(t *testing.T) {
		dir := recFixture(t, history, keys, 0, true)
		e, took := timedOpen(t, dir)
		defer e.Close()
		t.Logf("snapshot, no tail: recovery took %v, replayed %d records", took, e.recovery.Records)
		if e.recovery.Records != 0 {
			t.Fatalf("replayed %d records, want 0", e.recovery.Records)
		}
		if sz := truncWALSize(t, dir); sz != wal.HeaderSize {
			t.Fatalf("WAL size = %d, want header only (%d)", sz, wal.HeaderSize)
		}
		if got, err := e.Get("key-7"); err != nil || !strings.HasPrefix(string(got), "value-") {
			t.Fatalf("key-7 = %q, %v", got, err)
		}
	})
}

// BenchmarkRecovery times NewEngine on a fixed on-disk state. Opening
// repairs nothing and writes nothing, so reopening in a loop is repeatable.
func BenchmarkRecovery(b *testing.B) {
	const keys = 100
	cases := []struct {
		name          string
		history, tail int
		snapshot      bool
	}{
		{"nosnap/history=20000", 20_000, 0, false},
		{"snap/history=2000/tail=100", 2_000, 100, true},
		{"snap/history=20000/tail=100", 20_000, 100, true},
		{"snap/history=20000/tail=1000", 20_000, 1_000, true},
	}

	// Build every fixture once, up front: b.Run calls its function repeatedly
	// while it scales b.N, and the setup is far too slow to repeat.
	dirs := make([]string, len(cases))
	for i, c := range cases {
		dirs[i] = recFixture(b, c.history, keys, c.tail, c.snapshot)
	}

	for i, c := range cases {
		dir := dirs[i]
		b.Run(c.name, func(b *testing.B) {
			var replayed int
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				e, err := NewEngine(filepath.Join(dir, "wal.log"))
				if err != nil {
					b.Fatal(err)
				}
				replayed = e.recovery.Records
				e.Close()
			}
			b.StopTimer()
			b.ReportMetric(float64(replayed), "records/open")
		})
	}
	_ = os.Stdout // keep imports stable if you trim cases
}
