package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Ashrjz/raftkv/internal/wal"
)

func TestEngineLogBeforeApply(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")

	w, err := wal.Open(walPath)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	defer w.Close()

	e := NewEngine(w)

	// 1. Put succeeds normally
	if err := e.Put("key1", []byte("val1")); err != nil {
		t.Fatalf("first Put failed: %v", err)
	}

	got, _ := e.Get("key1")
	if string(got) != "val1" {
		t.Fatal("key1 should be in memory after successful Put")
	}

	// 2. Close the WAL to simulate a broken state
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close wal: %v", err)
	}

	// 3. Try Put on closed WAL: should fail
	if err := e.Put("key2", []byte("val2")); err == nil {
		t.Fatal("Put should have failed on closed WAL")
	}

	// 4. CRITICAL: key2 must NOT be in memory
	if _, err := e.Get("key2"); err == nil {
		t.Fatal("key2 should NOT be in memory after failed Put")
	}

	// 5. Verify key1 still exists (earlier writes survived)
	got, _ = e.Get("key1")
	if string(got) != "val1" {
		t.Fatal("earlier writes should still be readable")
	}

}

func TestEngineDelete_LogBeforeApply(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")

	w, err := wal.Open(walPath)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	defer w.Close()

	e := NewEngine(w)

	// Put a key first
	if err := e.Put("k", []byte("v")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify it's there
	if _, err := e.Get("k"); err != nil {
		t.Fatal("key should exist after Put")
	}

	// Close WAL to break it
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close wal: %v", err)
	}

	// Delete should fail
	if err := e.Delete("k"); err == nil {
		t.Fatal("Delete should have failed on closed WAL")
	}

	// Key should still exist in memory (Delete was not applied)
	if _, err := e.Get("k"); err != nil {
		t.Fatal("key should still exist after failed Delete")
	}
}

func TestEngineGet_IsLocal(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")

	w, err := wal.Open(walPath)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	defer w.Close()

	e := NewEngine(w)

	// Put some data
	if err := e.Put("k", []byte("v")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Close WAL
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close wal: %v", err)
	}

	// Get should still work (reads only from memory, no WAL involved)
	got, err := e.Get("k")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got) != "v" {
		t.Fatal("Get should return correct value")
	}
}

func TestEnginePutMultiple(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")

	w, err := wal.Open(walPath)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	defer w.Close()

	e := NewEngine(w)

	// Put multiple keys
	for i := 0; i < 10; i++ {
		key := "key" + string(rune(i))
		val := []byte("val" + string(rune(i)))
		if err := e.Put(key, val); err != nil {
			t.Fatalf("Put %s failed for value %v: %v", key, val, err)
		}
	}

	// Verify all are in memory
	for i := 0; i < 10; i++ {
		key := "key" + string(rune(i))
		expected := "val" + string(rune(i))
		got, err := e.Get(key)
		if err != nil || string(got) != expected {
			t.Fatalf("expected %s, got %s (err: %v)", expected, got, err)
		}
	}

	// Verify WAL grew
	fi, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("failed to stat wal: %v", err)
	}
	if fi.Size() <= 16 {
		t.Errorf("expected WAL size > 16 bytes after 10 Puts, got %d", fi.Size())
	}
}
