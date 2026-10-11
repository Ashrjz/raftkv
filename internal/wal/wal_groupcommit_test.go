package wal

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func TestGroupCommitConcurrentAppendsAreDurable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := Open(path, WithGroupCommit())
	if err != nil {
		t.Fatal(err)
	}

	const goroutines, per = 8, 100
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				rec := Record{
					Type:  TypePut,
					Key:   []byte(fmt.Sprintf("g%d-%d", g, i)),
					Value: []byte("v"),
				}
				if err := w.Append(rec); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	var wantEnd uint64
	for g := 0; g < goroutines; g++ {
		for i := 0; i < per; i++ {
			wantEnd += offSize(t, Record{
				Type: TypePut, Key: []byte(fmt.Sprintf("g%d-%d", g, i)), Value: []byte("v"),
			})
		}
	}
	if got := w.LogicalEnd(); got != wantEnd {
		t.Fatalf("LogicalEnd = %d, want %d", got, wantEnd)
	}
	batches, records := w.Stats()
	if records != goroutines*per || batches == 0 || batches > records {
		t.Fatalf("stats: batches=%d records=%d", batches, records)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Everything acknowledged is on disk, and each goroutine's own records
	// appear in the order it issued them.
	_, got, _, err := offRecover(t, path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != goroutines*per {
		t.Fatalf("recovered %d records, want %d", len(got), goroutines*per)
	}
	next := make([]int, goroutines)
	for _, r := range got {
		var g, i int
		if _, err := fmt.Sscanf(string(r.Key), "g%d-%d", &g, &i); err != nil {
			t.Fatalf("bad key %q: %v", r.Key, err)
		}
		if i != next[g] {
			t.Fatalf("goroutine %d: record %d appeared where %d was expected", g, i, next[g])
		}
		next[g]++
	}
}

func TestGroupCommitCloseAndAppendAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := Open(path, WithGroupCommit())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(offPut(0)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(offPut(1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("append after close: err = %v, want ErrClosed", err)
	}
	w.Close() // a second Close must not hang or panic; its error is irrelevant

	_, got, _, err := offRecover(t, path, 0)
	if err != nil {
		t.Fatal(err)
	}
	offCheckRecords(t, got, 0, 1)
}

func TestGroupCommitWithConcurrentTruncation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := Open(path, WithGroupCommit())
	if err != nil {
		t.Fatal(err)
	}

	const goroutines, per = 4, 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if err := w.Append(offPut(g*per + i)); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}

	stop := make(chan struct{})
	truncDone := make(chan struct{})
	go func() {
		defer close(truncDone)
		for {
			select {
			case <-stop:
				return
			default:
				if err := w.TruncateBefore(w.LogicalEnd()); err != nil {
					t.Error(err)
					return
				}
			}
		}
	}()

	wg.Wait()
	close(stop)
	<-truncDone

	var wantEnd uint64
	for n := 0; n < goroutines*per; n++ {
		wantEnd += offSize(t, offPut(n))
	}
	if got := w.LogicalEnd(); got != wantEnd {
		t.Fatalf("LogicalEnd = %d, want %d", got, wantEnd)
	}
	w.Close()

	// No bytes were lost or duplicated across the racing truncations: the
	// file replays cleanly from its base and ends at the same logical offset.
	base := truncHeaderBase(t, path)
	w2, _, _, err := offRecover(t, path, base)
	if err != nil {
		t.Fatal(err)
	}
	if w2.LogicalEnd() != wantEnd {
		t.Fatalf("LogicalEnd after reopen = %d, want %d", w2.LogicalEnd(), wantEnd)
	}
}
