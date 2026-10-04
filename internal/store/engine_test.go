package store

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Ashrjz/raftkv/internal/wal"
)

// openEngine opens (or reopens) an Engine at path and guarantees the WAL file
// handle is released before t.TempDir() cleanup runs (required on Windows).
// Calling e.Close() again inside a test is fine; the cleanup ignores the error.
func openEngine(t *testing.T, path string) *Engine {
	t.Helper()
	e, err := NewEngine(path)
	if err != nil {
		t.Fatalf("NewEngine(%q) failed: %v", path, err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func walPathIn(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.wal") // TempDir registered BEFORE openEngine's cleanup
}

// ---------- Write path: log before apply ----------

func TestEngineLogBeforeApply(t *testing.T) {
	e := openEngine(t, walPathIn(t))

	if err := e.Put("key1", []byte("val1")); err != nil {
		t.Fatalf("first Put failed: %v", err)
	}
	got, err := e.Get("key1")
	if err != nil || string(got) != "val1" {
		t.Fatalf("key1 should be readable after Put, got %q (err %v)", got, err)
	}

	// Closing the WAL makes every later Append fail.
	if err := e.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if err := e.Put("key2", []byte("val2")); err == nil {
		t.Fatal("Put should have failed on closed WAL")
	}

	// CRITICAL: a failed log write must not reach memory.
	if _, err := e.Get("key2"); err == nil {
		t.Fatal("key2 must NOT be in memory after failed Put")
	}

	got, err = e.Get("key1")
	if err != nil || string(got) != "val1" {
		t.Fatalf("earlier write should survive, got %q (err %v)", got, err)
	}
}

func TestEngineDelete_LogBeforeApply(t *testing.T) {
	e := openEngine(t, walPathIn(t))

	if err := e.Put("k", []byte("v")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if _, err := e.Get("k"); err != nil {
		t.Fatal("key should exist after Put")
	}

	if err := e.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if err := e.Delete("k"); err == nil {
		t.Fatal("Delete should have failed on closed WAL")
	}

	// The delete was never logged, so it must not be applied.
	if _, err := e.Get("k"); err != nil {
		t.Fatal("key should still exist after failed Delete")
	}
}

func TestEngineGet_IsLocal(t *testing.T) {
	e := openEngine(t, walPathIn(t))

	if err := e.Put("k", []byte("v")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reads never touch the WAL.
	got, err := e.Get("k")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got) != "v" {
		t.Fatalf("got %q want %q", got, "v")
	}
}

func TestEnginePutMultiple(t *testing.T) {
	path := walPathIn(t)
	e := openEngine(t, path)

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Appendf(nil, "val%d", i)
		if err := e.Put(key, val); err != nil {
			t.Fatalf("Put %s failed: %v", key, err)
		}
	}

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		want := fmt.Sprintf("val%d", i)
		got, err := e.Get(key)
		if err != nil || string(got) != want {
			t.Fatalf("key %s: got %q want %q (err %v)", key, got, want, err)
		}
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if fi.Size() <= wal.HeaderSize {
		t.Errorf("expected WAL larger than header (%d) after 10 Puts, got %d", wal.HeaderSize, fi.Size())
	}
}

// ---------- Replay ----------

func TestEngineReplayFromDisk(t *testing.T) {
	path := walPathIn(t)

	e1 := openEngine(t, path)
	if err := e1.Put("k1", []byte("v1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := e1.Put("k2", []byte("v2")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	e2 := openEngine(t, path) // replays the WAL

	for k, want := range map[string]string{"k1": "v1", "k2": "v2"} {
		got, err := e2.Get(k)
		if err != nil || string(got) != want {
			t.Fatalf("after replay %s: got %q want %q (err %v)", k, got, want, err)
		}
	}
}

func TestEngineReplayWithDelete(t *testing.T) {
	path := walPathIn(t)

	e1 := openEngine(t, path)
	mustPut(t, e1, "k1", []byte("v1"))
	mustPut(t, e1, "k2", []byte("v2"))
	if err := e1.Delete("k1"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	e2 := openEngine(t, path)

	if _, err := e2.Get("k1"); err == nil {
		t.Fatal("k1 must not exist after Delete + replay")
	}
	got, err := e2.Get("k2")
	if err != nil || string(got) != "v2" {
		t.Fatalf("k2: got %q (err %v)", got, err)
	}
}

func TestEngineReplayOverwriteLastWins(t *testing.T) {
	path := walPathIn(t)

	e1 := openEngine(t, path)
	mustPut(t, e1, "k", []byte("old"))
	mustPut(t, e1, "k", []byte("new"))
	if err := e1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	e2 := openEngine(t, path)
	got, err := e2.Get("k")
	if err != nil || string(got) != "new" {
		t.Fatalf("got %q (err %v), want %q: replay must apply records in log order", got, err, "new")
	}
}

func TestEngineReplayDeleteThenPut(t *testing.T) {
	path := walPathIn(t)

	e1 := openEngine(t, path)
	mustPut(t, e1, "k", []byte("v1"))
	if err := e1.Delete("k"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	mustPut(t, e1, "k", []byte("v2"))
	if err := e1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	e2 := openEngine(t, path)
	got, err := e2.Get("k")
	if err != nil || string(got) != "v2" {
		t.Fatalf("got %q (err %v), want v2", got, err)
	}
}

func TestEngineEmptyReplay(t *testing.T) {
	path := walPathIn(t)

	e1 := openEngine(t, path)
	if err := e1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	e2 := openEngine(t, path) // header-only WAL must replay to empty state
	if _, err := e2.Get("anything"); err == nil {
		t.Fatal("empty engine should have no keys")
	}
	if err := e2.Put("k", []byte("v")); err != nil {
		t.Fatalf("Put after empty replay failed: %v", err)
	}
}

// Writes made AFTER a replay must be appended correctly and survive another restart.
func TestEngineWriteAfterReplay(t *testing.T) {
	path := walPathIn(t)

	e1 := openEngine(t, path)
	mustPut(t, e1, "a", []byte("1"))
	if err := e1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	e2 := openEngine(t, path)
	mustPut(t, e2, "b", []byte("2"))
	if err := e2.Delete("a"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if err := e2.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	e3 := openEngine(t, path)
	if _, err := e3.Get("a"); err == nil {
		t.Fatal("a should be deleted after second restart")
	}
	got, err := e3.Get("b")
	if err != nil || string(got) != "2" {
		t.Fatalf("b: got %q (err %v)", got, err)
	}
}

// A record far larger than the 64 KiB read buffer must replay intact.
func TestEngineReplayLargeRecord(t *testing.T) {
	path := walPathIn(t)
	big := bytes.Repeat([]byte("x"), 200*1024)

	e1 := openEngine(t, path)
	mustPut(t, e1, "big", big)
	mustPut(t, e1, "after", []byte("still-here")) // record following the big one
	if err := e1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	e2 := openEngine(t, path)
	got, err := e2.Get("big")
	if err != nil || !bytes.Equal(got, big) {
		t.Fatalf("big value mismatch: len=%d err=%v", len(got), err)
	}
	got, err = e2.Get("after")
	if err != nil || string(got) != "still-here" {
		t.Fatalf("record after the big one lost: %q (err %v)", got, err)
	}
}

// Many variable-size records so that many straddle 64 KiB read-buffer boundaries.
// (fsync-per-write makes this the slowest test; that cost is what group commit fixes later.)
func TestEngineReplayAcrossChunkBoundaries(t *testing.T) {
	path := walPathIn(t)
	const n = 1500

	valueFor := func(i int) []byte { return bytes.Repeat([]byte{byte(i)}, i%700) }

	e1 := openEngine(t, path)
	for i := 0; i < n; i++ {
		mustPut(t, e1, fmt.Sprintf("key-%05d", i), valueFor(i))
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if fi, err := os.Stat(path); err != nil || fi.Size() < 3*64*1024 {
		t.Fatalf("test should span several 64 KiB chunks, size=%v err=%v", fi, err)
	}

	e2 := openEngine(t, path)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%05d", i)
		got, err := e2.Get(key)
		if err != nil {
			t.Fatalf("%s missing after replay: %v", key, err)
		}
		if !bytes.Equal(got, valueFor(i)) {
			t.Fatalf("%s: value mismatch (len got=%d want=%d)", key, len(got), i%700)
		}
	}
}

func TestEngineRecoversFromTornTail(t *testing.T) {
	path := walPathIn(t)

	e1 := openEngine(t, path)
	mustPut(t, e1, "a", []byte("1"))
	mustPut(t, e1, "b", []byte("2"))
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-append: 6 stray bytes, fewer than a record prefix.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	e2 := openEngine(t, path)
	if e2.recovery.DiscardedBytes != 6 {
		t.Fatalf("discarded %d, want 6", e2.recovery.DiscardedBytes)
	}
	for k, want := range map[string]string{"a": "1", "b": "2"} {
		if got, err := e2.Get(k); err != nil || string(got) != want {
			t.Fatalf("%s: got %q err %v", k, got, err)
		}
	}
	mustPut(t, e2, "c", []byte("3"))
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}

	e3 := openEngine(t, path) // a second restart must be clean
	if e3.recovery.DiscardedBytes != 0 {
		t.Fatalf("second recovery discarded %d bytes", e3.recovery.DiscardedBytes)
	}
	if got, err := e3.Get("c"); err != nil || string(got) != "3" {
		t.Fatalf("c: got %q err %v", got, err)
	}
}

func mustPut(t *testing.T, e *Engine, k string, v []byte) {
	t.Helper()
	if err := e.Put(k, v); err != nil {
		t.Fatalf("Put(%q) failed: %v", k, err)
	}
}
